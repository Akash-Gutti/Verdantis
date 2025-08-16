@echo off
cd /d %~dp0\..\..
docker compose -f compose.yml up -d
echo Stack up: PostGIS on 5432, Redis on 6379, MinIO on 9000/9001
