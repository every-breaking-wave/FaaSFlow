
workflow_name="image-processing"
kubectl get pods --no-headers=true -n function | grep "^"$workflow_name | awk '{print $1}' | xargs kubectl delete pod