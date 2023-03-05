from prefect.deployments import Deployment
from etl_web_to_gcs import etl_web_to_gcs
from prefect.filesystems import GitHub 

github_block = GitHub.load("github-zoom")

deployment = Deployment.build_from_flow(
     flow=etl_web_to_gcs,
     name="github-deployment",
     storage=github_block,
     entrypoint="prefect/etl_web_to_gcs.py:etl_web_to_gcs")

if __name__ == "__main__":
    deployment.apply()