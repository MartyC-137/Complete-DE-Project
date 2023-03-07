from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/MartyC-137/dbt-core_GitHubActions-pipeline",
)
block.get_directory("prefect") # specify a subfolder of repo
block.save("github", overwrite = True)

print('Github creds block successfully configured!', '\n')
