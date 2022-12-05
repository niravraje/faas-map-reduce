
# Deploy Master
gcloud functions deploy master-cf \
--gen2 \
--runtime=python310 \
--region=us-central1 \
--source=. \
--entry-point=master_init \
--trigger-http \
--allow-unauthenticated

# Deploy mapper faas
gcloud functions deploy mapper-cf \
--gen2 \
--runtime=python310 \
--region=us-central1 \
--source=. \
--entry-point=mapper_init \
--trigger-http \
--allow-unauthenticated

# Deploy reducer faas
gcloud functions deploy reducer-cf \
--gen2 \
--runtime=python310 \
--region=us-central1 \
--source=. \
--entry-point=reducer_init \
--trigger-http \
--allow-unauthenticated

# Deploy UI handler
gcloud functions deploy ui-handler-cf \
--gen2 \
--runtime=python310 \
--region=us-central1 \
--source=. \
--entry-point=handle_request \
--trigger-http \
--allow-unauthenticated

# Deploy UI
gcloud functions deploy web-ui-cf \
--gen2 \
--runtime=python310 \
--region=us-central1 \
--source=. \
--entry-point=launch_ui \
--trigger-http \
--allow-unauthenticated

