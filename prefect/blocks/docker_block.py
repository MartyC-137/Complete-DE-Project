from prefect.infrastructure.docker import DockerContainer

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image = 'martyp303/prefect:zoom',
    image_pull_policy = "ALWAYS",
    auto_remove = True,
)

docker_block.save("zoom-docker", overwrite = True)

print('Docker block successfully configured!', '\n')