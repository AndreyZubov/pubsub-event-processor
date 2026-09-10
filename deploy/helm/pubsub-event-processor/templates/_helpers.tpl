{{/*
Chart name, overridable via nameOverride.
*/}}
{{- define "processor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified release name. Kubernetes names are capped at 63 characters.
*/}}
{{- define "processor.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "processor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Labels recommended by Kubernetes. Applied to every object.
*/}}
{{- define "processor.labels" -}}
helm.sh/chart: {{ include "processor.chart" . }}
{{ include "processor.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: {{ include "processor.name" . }}
{{- end }}

{{/*
Selector labels. These are immutable on a Deployment, so they must stay
minimal — never add version here, or upgrades will fail.
*/}}
{{- define "processor.selectorLabels" -}}
app.kubernetes.io/name: {{ include "processor.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "processor.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "processor.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding Salesforce credentials and the database DSN.
Either the one this chart creates, or a pre-existing one supplied by the user.
*/}}
{{- define "processor.secretName" -}}
{{- if .Values.secrets.existingSecret }}
{{- .Values.secrets.existingSecret }}
{{- else }}
{{- include "processor.fullname" . }}
{{- end }}
{{- end }}

{{- define "processor.postgres.fullname" -}}
{{- printf "%s-postgres" (include "processor.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Database DSN. When the bundled Postgres is enabled the DSN points at its
in-cluster Service; otherwise the user supplies one through secrets.databaseUrl
or an existing Secret.
*/}}
{{- define "processor.databaseUrl" -}}
{{- if .Values.postgres.enabled -}}
postgres://{{ .Values.postgres.auth.username }}:{{ .Values.postgres.auth.password }}@{{ include "processor.postgres.fullname" . }}:5432/{{ .Values.postgres.auth.database }}?sslmode=disable
{{- else -}}
{{- required "Set postgres.enabled=true, or provide secrets.databaseUrl / secrets.existingSecret" .Values.secrets.databaseUrl -}}
{{- end -}}
{{- end }}
