/* Copyright (c) 2017 Parallels IP Holdings GmbH */

// Api versions allow the api contract for a resource to be changed while keeping
// backward compatibility by support multiple concurrent versions
// of the same resource

// +k8s:openapi-gen=true
// +k8s:deepcopy-gen=package,register
// +k8s:conversion-gen=git.sw.ru/ap/ap-api-snapshots/pkg/apis/snapshots
// +k8s:defaulter-gen=TypeMeta
// +groupName=snapshots.ap.virtuozzo.com
package v1 // import "git.sw.ru/ap/ap-api-snapshots/pkg/apis/snapshots/v1"
