#!/bin/bash

STREAMS=('tutorial.inventory.customers'
  'tutorial.inventory.products'
  'tutorial.inventory.geom'
  'tutorial.inventory.products_on_hand'
  'tutorial.inventory.orders')
REGION=eu-central-1

for stream in ${STREAMS[@]}
do
  echo "Creating stream '$stream' in Kinesis"
  docker exec kinesis-localstack-1 awslocal kinesis create-stream --stream-name $stream --region $REGION
done

echo "List all the streams in Kinesis"
docker exec kinesis-localstack-1 awslocal kinesis list-streams --region $REGION