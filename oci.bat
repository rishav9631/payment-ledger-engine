@echo off
docker run --rm -v "%USERPROFILE%\.oci:/oracle/.oci" -v "%USERPROFILE%\.ssh:/oracle/.ssh" -v "%CD%:/workspace" -w /workspace ghcr.io/oracle/oci-cli %*
