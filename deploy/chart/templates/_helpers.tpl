{{/* The stable service identity (architecture.md naming): Deployment,
ServiceAccount and Helm release share it. */}}
{{- define "anvilkit-agent-workflow.name" -}}
anvilkit-agent-workflow
{{- end -}}

{{/* A release named after the service, or after the service with a suffix
(the development foundation renders the launcher identity of a worker
outside the cluster as "anvilkit-agent-workflow-host"), keeps its name;
any other release name is prefixed. */}}
{{- define "anvilkit-agent-workflow.fullname" -}}
{{- if hasPrefix (include "anvilkit-agent-workflow.name" .) .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "anvilkit-agent-workflow.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-workflow.labels" -}}
app.kubernetes.io/name: {{ include "anvilkit-agent-workflow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/component: workflow
app.kubernetes.io/part-of: anvilkit
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{- define "anvilkit-agent-workflow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "anvilkit-agent-workflow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "anvilkit-agent-workflow.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "anvilkit-agent-workflow.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* The image reference: a digest pins the exact image, otherwise the tag
(defaulting to the chart's appVersion). */}}
{{- define "anvilkit-agent-workflow.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{/* The Worker Build ID: explicit, else the pinned digest, else the tag. */}}
{{- define "anvilkit-agent-workflow.buildId" -}}
{{- if .Values.buildId -}}
{{- .Values.buildId -}}
{{- else if .Values.image.digest -}}
{{- .Values.image.digest -}}
{{- else -}}
{{- default .Chart.AppVersion .Values.image.tag -}}
{{- end -}}
{{- end -}}

{{/* P1.7 Worker Deployment Versioning (config.temporal.versioning.enabled):
a release renders one worker version. Its Deployment, ConfigMap, HPA and
PodDisruptionBudget are named and selected per Build ID, so the release of
build B starts B's workers beside build A's instead of replacing them, and
they carry helm.sh/resource-policy keep and Argo CD's Prune=false, so
neither Helm nor Argo CD deletes A's objects when B's release no longer
renders them. The rollout removes them once A's pinned runs have drained
(deploy/qualification/workflow-rollout.py of the deploying repository).
The ServiceAccount, RBAC and certificates stay shared by the release.
Without versioning every name, label and selector is the unversioned one. */}}
{{- define "anvilkit-agent-workflow.versioned" -}}
{{- if dig "temporal" "versioning" "enabled" false .Values.config -}}true{{- end -}}
{{- end -}}

{{/* The label-safe key of this worker version: the first 10 hex digits of
the Build ID's SHA-256 (a digest Build ID exceeds a label value). */}}
{{- define "anvilkit-agent-workflow.versionKey" -}}
{{- include "anvilkit-agent-workflow.buildId" . | sha256sum | trunc 10 -}}
{{- end -}}

{{/* The name of the per-version objects: the fullname, followed by the
version key under versioning. */}}
{{- define "anvilkit-agent-workflow.workerName" -}}
{{- if include "anvilkit-agent-workflow.versioned" . -}}
{{- printf "%s-%s" (include "anvilkit-agent-workflow.fullname" . | trunc 52 | trimSuffix "-") (include "anvilkit-agent-workflow.versionKey" .) -}}
{{- else -}}
{{- include "anvilkit-agent-workflow.fullname" . -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-workflow.workerLabels" -}}
{{ include "anvilkit-agent-workflow.labels" . }}
{{- if include "anvilkit-agent-workflow.versioned" . }}
anvilkit.io/worker-version: {{ include "anvilkit-agent-workflow.versionKey" . }}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-workflow.workerSelectorLabels" -}}
{{ include "anvilkit-agent-workflow.selectorLabels" . }}
{{- if include "anvilkit-agent-workflow.versioned" . }}
anvilkit.io/worker-version: {{ include "anvilkit-agent-workflow.versionKey" . }}
{{- end -}}
{{- end -}}

{{/* The annotations of the per-version objects under versioning (empty
otherwise): the Temporal version they serve and the keep policies. */}}
{{- define "anvilkit-agent-workflow.workerAnnotations" -}}
{{- if include "anvilkit-agent-workflow.versioned" . -}}
anvilkit.io/worker-deployment: {{ required "config.temporal.versioning.deployment_name is required under versioning" .Values.config.temporal.versioning.deployment_name | quote }}
anvilkit.io/worker-build-id: {{ include "anvilkit-agent-workflow.buildId" . | quote }}
helm.sh/resource-policy: keep
argocd.argoproj.io/sync-options: Prune=false
{{- end -}}
{{- end -}}

{{/* Required environment values of the worker Deployment. */}}
{{- define "anvilkit-agent-workflow.require" -}}
{{- /* P2.2: the filesystem Pagix/Knowledge double is DEVELOPMENT_ONLY. */ -}}
{{- if and (dig "lifecycle" "pagix_double_dir" "" .Values.config) (not .Values.development.enabled) }}
{{- fail "config.lifecycle.pagix_double_dir is the DEVELOPMENT_ONLY Pagix/Knowledge double: it renders only with development.enabled: true" }}
{{- end }}
{{- if and .Values.pagix.declarationConfigMap (not .Values.pagix.baseUrl) }}
{{- fail "pagix.baseUrl is required with pagix.declarationConfigMap (ANVILKIT_WORKFLOW_PAGIX_URL)" }}
{{- end }}
{{- if and .Values.pagix.declarationConfigMap (not (has .Values.pagix.identity (list "mtls" "token"))) }}
{{- fail "pagix.identity must be mtls or token" }}
{{- end }}
{{- if not .Values.temporal.address }}
{{- fail "temporal.address is required: the Temporal frontend (ANVILKIT_WORKFLOW_TEMPORAL_ADDRESS)" }}
{{- end }}
{{- if not .Values.control.address }}
{{- fail "control.address is required: the Control gRPC endpoint (ANVILKIT_WORKFLOW_CONTROL_ADDRESS)" }}
{{- end }}
{{- if not .Values.launcher.backend }}
{{- fail "launcher.backend is required: the launch backend identity Control records (ANVILKIT_WORKFLOW_LAUNCH_BACKEND)" }}
{{- end }}
{{- if not .Values.launcher.imageRegistry }}
{{- fail "launcher.imageRegistry is required: the registry that completes profile images (ANVILKIT_WORKFLOW_IMAGE_REGISTRY)" }}
{{- end }}
{{- if not .Values.launcher.sidecarControlAddress }}
{{- fail "launcher.sidecarControlAddress is required: Control as a Job Pod's access sidecar reaches it (ANVILKIT_WORKFLOW_SIDECAR_CONTROL_ADDRESS)" }}
{{- end }}
{{- if not (has .Values.identity.mode (list "mtls" "development")) }}
{{- fail "identity.mode must be mtls or development" }}
{{- end }}
{{- if and (eq .Values.identity.mode "development") (not .Values.development.enabled) }}
{{- fail "identity.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true (plaintext clients present no workload identity)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") (not .Values.identity.trustDomain) (not .Values.development.enabled) }}
{{- fail "identity.trustDomain is required outside development (the development default anvilkit.local applies only with development.enabled: true)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") .Values.identity.certificate.create (not .Values.identity.certificate.issuerRef.name) }}
{{- fail "identity.certificate.issuerRef.name is required: the cert-manager issuer of the workload certificate (or set identity.certificate.create false and identity.secretName)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") (not .Values.identity.certificate.create) (not .Values.identity.secretName) }}
{{- fail "identity.secretName is required while identity.certificate.create is false" }}
{{- end }}
{{- if not (has .Values.temporal.tls.mode (list "tls" "mtls" "development")) }}
{{- fail "temporal.tls.mode must be tls, mtls or development" }}
{{- end }}
{{- if and (eq .Values.temporal.tls.mode "development") (not .Values.development.enabled) }}
{{- fail "temporal.tls.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true" }}
{{- end }}
{{- if and (ne .Values.temporal.tls.mode "development") (not .Values.temporal.tls.caSecret.name) }}
{{- fail "temporal.tls.caSecret.name is required under temporal.tls.mode tls or mtls: the Secret holding the frontend's CA bundle (key ca.crt)" }}
{{- end }}
{{- if and (eq .Values.temporal.tls.mode "mtls") (ne .Values.identity.mode "mtls") }}
{{- fail "temporal.tls.mode mtls presents the workload certificate and needs identity.mode mtls" }}
{{- end }}
{{- if not (has .Values.sidecar.identityMode (list "mtls" "development")) }}
{{- fail "sidecar.identityMode must be mtls or development" }}
{{- end }}
{{- if and (eq .Values.sidecar.identityMode "development") (not .Values.development.enabled) }}
{{- fail "sidecar.identityMode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true (a plaintext sidecar)" }}
{{- end }}
{{- if and (eq .Values.sidecar.identityMode "mtls") (not .Values.sidecar.identitySecret) }}
{{- fail "sidecar.identitySecret is required under sidecar.identityMode mtls" }}
{{- end }}
{{- if and (eq .Values.sidecar.identityMode "mtls") (ne .Values.identity.mode "mtls") }}
{{- fail "sidecar.identityMode mtls needs identity.mode mtls: the sidecar's certificate comes from the same issuer" }}
{{- end }}
{{- if .Values.telemetry.otlpEndpoint }}
{{- if not (has .Values.telemetry.otlpTls.mode (list "tls" "mtls" "development")) }}
{{- fail "telemetry.otlpTls.mode must be tls, mtls or development" }}
{{- end }}
{{- if and (eq .Values.telemetry.otlpTls.mode "development") (not .Values.development.enabled) }}
{{- fail "telemetry.otlpTls.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true" }}
{{- end }}
{{- if and (ne .Values.telemetry.otlpTls.mode "development") (not .Values.telemetry.otlpTls.caSecret.name) }}
{{- fail "telemetry.otlpTls.caSecret.name is required under telemetry.otlpTls.mode tls or mtls" }}
{{- end }}
{{- end }}
{{- end -}}

{{/* The identity Secret: the rendered Certificate's or the environment's. */}}
{{- define "anvilkit-agent-workflow.identitySecret" -}}
{{- if .Values.identity.certificate.create -}}
{{- printf "%s-identity" (include "anvilkit-agent-workflow.fullname" .) -}}
{{- else -}}
{{- .Values.identity.secretName -}}
{{- end -}}
{{- end -}}

{{/* The rendered configuration: the reviewed sections plus the chart-owned
placement, identity, guard and transport settings, so every value above is
wired to the loader's keys. */}}
{{- define "anvilkit-agent-workflow.config" -}}
{{- $cfg := deepCopy .Values.config -}}
{{- $_ := set $cfg.kubernetes "namespace" .Values.launcher.namespace -}}
{{- $drain := int .Values.shutdown.drainSeconds -}}
{{- if or (lt $drain 1) (gt $drain 300) }}
{{- fail (printf "shutdown.drainSeconds %d is outside [1, 300], the worker's supported shutdown_timeout range" $drain) }}
{{- end }}
{{- $_ = set $cfg "shutdown_timeout" (printf "%ds" $drain) -}}
{{- $_ = set $cfg "development" (dict "enabled" .Values.development.enabled) -}}
{{- $_ = set $cfg.kubernetes "sidecar" (dict "identity_mode" .Values.sidecar.identityMode "identity_secret" .Values.sidecar.identitySecret) -}}
{{- if eq .Values.identity.mode "mtls" }}
{{- $_ = set $cfg "identity" (dict "cert_file" "/etc/anvilkit/identity/tls.crt" "key_file" "/etc/anvilkit/identity/tls.key" "ca_file" "/etc/anvilkit/identity/ca.crt") }}
{{- end -}}
{{- $_ = set $cfg.control "identity" (dict "mode" .Values.identity.mode "mtls" (dict "server_name" .Values.control.serverName)) -}}
{{- $mp := default (dict) $cfg.model_proxy -}}
{{- $_ = set $mp "identity" (dict "mode" .Values.identity.mode "mtls" (dict "server_name" .Values.modelProxy.serverName)) -}}
{{- $_ = set $cfg "model_proxy" $mp -}}
{{- if .Values.pagix.declarationConfigMap }}
{{- $lc := default (dict) $cfg.lifecycle -}}
{{- $p := dict "declaration_file" "/etc/anvilkit/pagix/declaration.yaml" "identity" .Values.pagix.identity -}}
{{- if .Values.pagix.serverName }}{{- $_ = set $p "server_name" .Values.pagix.serverName }}{{- end -}}
{{- if eq .Values.pagix.identity "token" }}{{- $_ = set $p "token_file" .Values.pagix.tokenFile }}{{- end -}}
{{- $_ = set $lc "pagix" $p -}}
{{- $_ = set $cfg "lifecycle" $lc -}}
{{- end -}}
{{- $tt := dict "mode" .Values.temporal.tls.mode -}}
{{- if ne .Values.temporal.tls.mode "development" }}
{{- $_ = set $tt "ca_file" "/etc/anvilkit/temporal-ca/ca.crt" }}
{{- if .Values.temporal.tls.serverName }}{{- $_ = set $tt "server_name" .Values.temporal.tls.serverName }}{{- end }}
{{- end -}}
{{- $_ = set $cfg.temporal "tls" $tt -}}
{{- if .Values.telemetry.otlpEndpoint }}
{{- $ot := dict "mode" .Values.telemetry.otlpTls.mode -}}
{{- if ne .Values.telemetry.otlpTls.mode "development" }}
{{- $_ = set $ot "ca_file" "/etc/anvilkit/otlp-ca/ca.crt" }}
{{- if .Values.telemetry.otlpTls.serverName }}{{- $_ = set $ot "server_name" .Values.telemetry.otlpTls.serverName }}{{- end }}
{{- end -}}
{{- $_ = set $cfg "telemetry" (merge (dict "otlp_tls" $ot) (default (dict) $cfg.telemetry)) -}}
{{- end -}}
{{- toYaml $cfg -}}
{{- end -}}
