{{- define "header-router.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "header-router.fullnameName" -}}
{{- if .Values.fullnameNameOverride }}
{{- .Values.fullnameNameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "header-router.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "header-router.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app: {{ include "header-router.name" . }}
{{- end }}

{{- define "header-router.selectorLabels" -}}
app.kubernetes.io/name: {{ include "header-router.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: {{ include "header-router.name" . }}
{{- end }}

{{- define "header-router.configMapName" -}}
{{ include "header-router.name" . }}-config
{{- end }}
