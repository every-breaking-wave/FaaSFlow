kubectl get pods --no-headers=true | grep "^image-processing" \
| awk '{print $1}' | xargs kubectl delete pod