/*
Copyright the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package install

import (
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/vmware-tanzu/velero/internal/velero"
)

// zhou: "cli install" will create and install this DaemonSet to run restic server

func DaemonSet(namespace string, opts ...podTemplateOption) *appsv1.DaemonSet {
	c := &podTemplateConfig{
		image: velero.DefaultVeleroImage(),
	}

	for _, opt := range opts {
		opt(c)
	}

	pullPolicy := corev1.PullAlways
	imageParts := strings.Split(c.image, ":")
	if len(imageParts) == 2 && imageParts[1] != "latest" {
		pullPolicy = corev1.PullIfNotPresent
	}

	daemonSetArgs := []string{
		"node-agent",
		"server",
	}
	if len(c.features) > 0 {
		daemonSetArgs = append(daemonSetArgs, fmt.Sprintf("--features=%s", strings.Join(c.features, ",")))
	}

	if len(c.nodeAgentConfigMap) > 0 {
		daemonSetArgs = append(daemonSetArgs, fmt.Sprintf("--node-agent-configmap=%s", c.nodeAgentConfigMap))
	}

	// zhou: work as root?

	userID := int64(0)
	mountPropagationMode := corev1.MountPropagationHostToContainer

	dsName := "node-agent"
	if c.forWindows {
		dsName = "node-agent-windows"
	}

	daemonSet := &appsv1.DaemonSet{
		ObjectMeta: objectMeta(namespace, dsName),
		TypeMeta: metav1.TypeMeta{
			Kind:       "DaemonSet",
			APIVersion: appsv1.SchemeGroupVersion.String(),
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"name": dsName,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: podLabels(c.labels, map[string]string{
						"name": dsName,
						"role": "node-agent",
					}),
					Annotations: c.annotations,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: c.serviceAccountName,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser: &userID,
					},
					// zhou: need to access host path "/var/lib/kubelet/pods"
					Volumes: []corev1.Volume{
						{
							Name: "host-pods",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/var/lib/kubelet/pods",
								},
							},
						},
						{
							Name: "host-plugins",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/var/lib/kubelet/plugins",
								},
							},
						},
						{
							Name: "scratch",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: new(corev1.EmptyDirVolumeSource),
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:            dsName,
							Image:           c.image,
							Ports:           containerPorts(),
							ImagePullPolicy: pullPolicy,
							Command: []string{
								"/velero",
							},
							Args: daemonSetArgs,
							SecurityContext: &corev1.SecurityContext{
								Privileged: &c.privilegedNodeAgent,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:             "host-pods",
									MountPath:        "/host_pods",
									MountPropagation: &mountPropagationMode,
								},
								{
									Name:             "host-plugins",
									MountPath:        "/var/lib/kubelet/plugins",
									MountPropagation: &mountPropagationMode,
								},
								{
									Name:      "scratch",
									MountPath: "/scratch",
								},
							},
							Env: []corev1.EnvVar{
								{
									Name: "NODE_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
								},
								{
									Name: "VELERO_NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
								{
									Name:  "VELERO_SCRATCH_DIR",
									Value: "/scratch",
								},
							},
							Resources: c.resources,
						},
					},
				},
			},
		},
	}

	// zhou: user specify "secret-file" in flags.
	if c.withSecret {
		daemonSet.Spec.Template.Spec.Volumes = append(
			daemonSet.Spec.Template.Spec.Volumes,
			corev1.Volume{
				Name: "cloud-credentials",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						// zhou: must be in same Namespace with this Pod
						SecretName: "cloud-credentials",
					},
				},
			},
		)
		// zhou: default sercet key "cloud" will be accessed via "/credentials/cloud"
		daemonSet.Spec.Template.Spec.Containers[0].VolumeMounts = append(
			daemonSet.Spec.Template.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{
				Name:      "cloud-credentials",
				MountPath: "/credentials",
			},
		)

		// zhou: set default value for these env, it could be overwrite when BSL specify
		//       private credential. But who will use the value???
		daemonSet.Spec.Template.Spec.Containers[0].Env = append(daemonSet.Spec.Template.Spec.Containers[0].Env, []corev1.EnvVar{
			{
				Name:  "GOOGLE_APPLICATION_CREDENTIALS",
				Value: "/credentials/cloud",
			},
			{
				Name:  "AWS_SHARED_CREDENTIALS_FILE",
				Value: "/credentials/cloud",
			},
			{
				Name:  "AZURE_CREDENTIALS_FILE",
				Value: "/credentials/cloud",
			},
			{
				Name:  "ALIBABA_CLOUD_CREDENTIALS_FILE",
				Value: "/credentials/cloud",
			},
		}...)
	}

	if c.forWindows {
		daemonSet.Spec.Template.Spec.SecurityContext = nil
		daemonSet.Spec.Template.Spec.Containers[0].SecurityContext = nil
		daemonSet.Spec.Template.Spec.NodeSelector = map[string]string{
			"kubernetes.io/os": "windows",
		}
		daemonSet.Spec.Template.Spec.OS = &corev1.PodOS{
			Name: "windows",
		}
		daemonSet.Spec.Template.Spec.Tolerations = []corev1.Toleration{
			{
				Key:      "os",
				Operator: "Equal",
				Effect:   "NoSchedule",
				Value:    "windows",
			},
		}
	} else {
		daemonSet.Spec.Template.Spec.NodeSelector = map[string]string{
			"kubernetes.io/os": "linux",
		}
		daemonSet.Spec.Template.Spec.OS = &corev1.PodOS{
			Name: "linux",
		}
	}

	daemonSet.Spec.Template.Spec.Containers[0].Env = append(daemonSet.Spec.Template.Spec.Containers[0].Env, c.envVars...)

	return daemonSet
}
