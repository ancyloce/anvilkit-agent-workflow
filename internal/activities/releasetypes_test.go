package activities

import (
	"strings"
	"testing"
)

// The contract vector of components/fixtures.json ("exact release subject").
func fixtureSubject() ReleaseSubject {
	h := func(c string) string { return "sha256:" + strings.Repeat(c, 64) }
	return ReleaseSubject{
		SchemaVersion: 1, ComponentID: "cmp_hero", PuckType: "Hero", SourceRevision: "3", SourceDigest: h("4"), PackageName: "@anvilkit/hero", Version: "1.0.0",
		Npm: ArtifactDigest{h("6"), "20480"}, Browser: ArtifactDigest{h("7"), "8192"}, CSS: []ArtifactDigest{{h("8"), "512"}},
		BuildProfileID: "build-support-v1", BuildProfileDigest: h("5"), ValidatorProfileID: "validator-v1", ValidatorProfileDigest: h("b"),
		HostAbi: "host-abi-v1", HostAbiDigest: h("c"),
		Destinations:                ReleaseDestinations{NpmRegistry: "https://registry.example.invalid/", BrowserOrigin: "https://components.example.invalid"},
		CertificationEvidenceDigest: h("9"),
	}
}

func TestSubjectDigestMatchesContractVector(t *testing.T) {
	s := fixtureSubject()
	got, err := ComputeSubjectDigest(s)
	if err != nil {
		t.Fatal(err)
	}
	if want := "sha256:ba0108acc45e339e7a8041e35a67fd50191f044216e4f6b8a46796554d5be75b"; got != want {
		t.Fatalf("subject digest %s, contract vector %s", got, want)
	}
	s.SubjectDigest = got
	again, _ := ComputeSubjectDigest(s)
	if again != got {
		t.Fatal("the digest must ignore subjectDigest itself")
	}
	s.Destinations.NpmRegistry = "https://other.example.invalid/"
	if changed, _ := ComputeSubjectDigest(s); changed == got {
		t.Fatal("a changed destination must change the subject digest")
	}
}
