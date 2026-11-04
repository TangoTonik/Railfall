# Data lake: one bucket with a prefix per medallion layer, and a pipeline user
# that can only read/write that bucket (no admin rights, no other buckets).

locals {
  lake_layers = ["bronze", "silver"]
}

resource "minio_s3_bucket" "lake" {
  bucket = var.lake_bucket
  acl    = "private"

  # Lake data is reproducible from the open-data sources, but never let a
  # `terraform destroy` silently wipe a populated bucket.
  force_destroy = false
}

# Tiny marker objects make the layer layout visible in the console and let the
# e2e workflow assert Terraform created the expected structure.
resource "minio_s3_object" "layer_marker" {
  for_each = toset(local.lake_layers)

  bucket_name = minio_s3_bucket.lake.bucket
  object_name = "${each.key}/.keep"
  content     = "managed by terraform\n"
}

data "minio_iam_policy_document" "pipeline" {
  statement {
    sid       = "ListLake"
    actions   = ["s3:GetBucketLocation", "s3:ListBucket"]
    resources = ["arn:aws:s3:::${minio_s3_bucket.lake.bucket}"]
  }

  statement {
    sid       = "ReadWriteLakeObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["arn:aws:s3:::${minio_s3_bucket.lake.bucket}/*"]
  }
}

resource "minio_iam_policy" "pipeline" {
  name   = "railfall-pipeline-rw"
  policy = data.minio_iam_policy_document.pipeline.json
}

resource "minio_iam_user" "pipeline" {
  name   = var.lake_pipeline_user
  secret = var.lake_pipeline_secret
}

resource "minio_iam_user_policy_attachment" "pipeline" {
  user_name   = minio_iam_user.pipeline.id
  policy_name = minio_iam_policy.pipeline.id
}
