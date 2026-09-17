// Package artifacts is the Workflow's trusted artifact port (DD-02 §6,
// P13): scoped transfers begun and finalized through Control's
// ArtifactService under stable command identities, the bytes moved
// through the capabilities Control issues (one PUT, one GET), the
// downloaded bytes verified against the digest and size Control answered.
// The object store's keys and credentials never reach this process; the
// capabilities are used once and never logged.
package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-workflow/internal/activities"
)

// Client implements activities.Artifacts.
type Client struct {
	artifacts controlv1.ArtifactServiceClient
	worker    string
	http      *http.Client
	window    time.Duration
}

// New builds the client over an established Control connection; window
// is the transfer deadline the Workflow sets on an upload it begins.
func New(conn *grpc.ClientConn, worker string, window time.Duration) *Client {
	if window <= 0 {
		window = 15 * time.Minute
	}
	return &Client{
		artifacts: controlv1.NewArtifactServiceClient(conn), worker: worker, window: window,
		http: &http.Client{Timeout: 5 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
}

var classes = map[string]controlv1.ArtifactClass{
	"prompt": controlv1.ArtifactClass_ARTIFACT_CLASS_PROMPT, "brief": controlv1.ArtifactClass_ARTIFACT_CLASS_BRIEF, "evidence": controlv1.ArtifactClass_ARTIFACT_CLASS_EVIDENCE,
	"answer": controlv1.ArtifactClass_ARTIFACT_CLASS_ANSWER, "argument": controlv1.ArtifactClass_ARTIFACT_CLASS_ARGUMENT,
}

func digestOf(b []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(b)) }

func (c *Client) command(tenantID, commandID string, input any) *controlv1.CommandIdentity {
	raw, _ := json.Marshal(input)
	return &controlv1.CommandIdentity{TenantId: tenantID, CommandId: commandID, ActorId: c.worker, RequestDigest: digestOf(raw)}
}

// Upload begins the transfer under commandID (":begin"), puts the bytes
// once through the capability and finalizes under commandID (":finalize")
// naming the object version the store answered. A reentered begin of an
// already finalized transfer answers the original transfer without a
// capability, and the finalize reenters the original outcome.
func (c *Client) Upload(ctx context.Context, tenantID, operationID, commandID, class, mediaType string, body []byte) (activities.ArtifactBinding, error) {
	cls, ok := classes[class]
	if !ok {
		return activities.ArtifactBinding{}, fmt.Errorf("artifact class %q is not one the Workflow uploads", class)
	}
	digest, size := digestOf(body), strconv.Itoa(len(body))
	deadline := time.Now().Add(c.window)
	begin := &controlv1.BeginTransferRequest{
		Scope: &controlv1.Scope{TenantId: tenantID, ActorId: c.worker}, Class: cls, ExpectedDigest: digest, ExpectedSize: size, MediaType: mediaType,
		OperationId: &operationID,
	}
	// The deadline is not part of the command digest: a reentry after a
	// lost receipt must reach the same transfer.
	begin.Command = c.command(tenantID, commandID+":begin", map[string]string{"class": class, "digest": digest, "size": size, "operation": operationID})
	begin.Deadline = timestamppb.New(deadline)
	resp, err := c.artifacts.BeginTransfer(ctx, begin)
	if err != nil {
		return activities.ArtifactBinding{}, fmt.Errorf("begin transfer: %w", err)
	}
	t := resp.GetTransfer()
	binding := activities.ArtifactBinding{TransferID: t.GetTransferId(), Digest: digest, Handle: t.GetHandle()}
	if t.GetState() == controlv1.TransferState_TRANSFER_STATE_FINALIZED {
		return binding, nil
	}
	up := resp.GetUpload()
	if up == nil || up.GetUrl() == "" {
		return activities.ArtifactBinding{}, fmt.Errorf("transfer %s is %s and carries no upload capability", t.GetTransferId(), t.GetState())
	}
	version, err := c.put(ctx, up, body)
	if err != nil {
		return activities.ArtifactBinding{}, err
	}
	fin := &controlv1.FinalizeTransferRequest{TransferId: t.GetTransferId(), ObjectVersion: version}
	fin.Command = c.command(tenantID, commandID+":finalize", map[string]string{"transfer": t.GetTransferId(), "version": version})
	if _, err := c.artifacts.FinalizeTransfer(ctx, fin); err != nil {
		return activities.ArtifactBinding{}, fmt.Errorf("finalize transfer: %w", err)
	}
	return binding, nil
}

// put performs the one request the capability authorizes and returns the
// object version the store answered.
func (c *Client) put(ctx context.Context, cap *controlv1.TransferCapability, body []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, cap.GetMethod(), cap.GetUrl(), bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.ContentLength = int64(len(body))
	for k, v := range cap.GetHeaders() {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("upload: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("upload answered %d", resp.StatusCode)
	}
	version := resp.Header.Get("x-amz-version-id")
	if version == "" {
		return "", errors.New("upload answered no object version; the bucket is not versioned")
	}
	return version, nil
}

// Read obtains the download capability under the reading operation's
// relationship and verifies the bytes against the digest and size Control
// answered before returning them.
func (c *Client) Read(ctx context.Context, operationID, transferID, handle string, maxBytes int64) ([]byte, activities.ArtifactBinding, error) {
	resp, err := c.artifacts.ReadArtifact(ctx, &controlv1.ReadArtifactRequest{TransferId: transferID, Handle: handle, OperationId: operationID})
	if err != nil {
		return nil, activities.ArtifactBinding{}, fmt.Errorf("read artifact: %w", err)
	}
	t := resp.GetTransfer()
	size, err := strconv.ParseInt(t.GetExpectedSize(), 10, 64)
	if err != nil || size < 0 {
		return nil, activities.ArtifactBinding{}, fmt.Errorf("transfer %s declares size %q", t.GetTransferId(), t.GetExpectedSize())
	}
	if maxBytes > 0 && size > maxBytes {
		return nil, activities.ArtifactBinding{}, fmt.Errorf("transfer %s holds %d bytes, the read is bounded to %d", t.GetTransferId(), size, maxBytes)
	}
	body, err := c.get(ctx, resp.GetDownload(), size)
	if err != nil {
		return nil, activities.ArtifactBinding{}, err
	}
	if got := digestOf(body); got != t.GetExpectedDigest() || int64(len(body)) != size {
		return nil, activities.ArtifactBinding{}, fmt.Errorf("transfer %s: downloaded bytes hash to %s (%d bytes), Control records %s (%d bytes)", t.GetTransferId(), got, len(body), t.GetExpectedDigest(), size)
	}
	return body, activities.ArtifactBinding{TransferID: t.GetTransferId(), Digest: t.GetExpectedDigest(), Handle: t.GetHandle()}, nil
}

func (c *Client) get(ctx context.Context, cap *controlv1.TransferCapability, size int64) ([]byte, error) {
	if cap == nil || cap.GetUrl() == "" {
		return nil, errors.New("read artifact: no download capability was issued")
	}
	req, err := http.NewRequestWithContext(ctx, cap.GetMethod(), cap.GetUrl(), nil)
	if err != nil {
		return nil, err
	}
	for k, v := range cap.GetHeaders() {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download answered %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, size+1))
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}
	return body, nil
}
