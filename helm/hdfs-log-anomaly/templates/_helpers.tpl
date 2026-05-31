{{- define "hdfs-log-anomaly.name" -}}
hdfs-log-anomaly
{{- end -}}

{{- define "hdfs-log-anomaly.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "hdfs-log-anomaly.name" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
