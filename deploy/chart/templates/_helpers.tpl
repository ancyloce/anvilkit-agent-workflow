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
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
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

{{/* Required environment values of the worker Deployment. */}}
{{- define "anvilkit-agent-workflow.require" -}}
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
{{- end -}}
