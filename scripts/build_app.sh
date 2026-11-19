#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_BUILD_DIR="${PROJECT_ROOT}/.artifacts/app"

npm --prefix "${PROJECT_ROOT}/frontend" ci
npm --prefix "${PROJECT_ROOT}/frontend" run build

if [[ ! -f "${PROJECT_ROOT}/frontend/dist/index.html" ]]; then
  echo "Frontend build did not produce frontend/dist/index.html" >&2
  exit 1
fi

rm -rf "${APP_BUILD_DIR}"
mkdir -p "${APP_BUILD_DIR}/frontend" "${APP_BUILD_DIR}/sql"

(
  cd "${PROJECT_ROOT}"
  tar \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='*.pyo' \
    -cf - backend
) | (cd "${APP_BUILD_DIR}" && tar -xf -)

cp -R "${PROJECT_ROOT}/frontend/dist" "${APP_BUILD_DIR}/frontend/dist"
cp "${PROJECT_ROOT}/sql/lakebase_schema.sql" "${APP_BUILD_DIR}/sql/lakebase_schema.sql"
cp "${PROJECT_ROOT}/pyproject.toml" "${APP_BUILD_DIR}/pyproject.toml"
cp "${PROJECT_ROOT}/uv.lock" "${APP_BUILD_DIR}/uv.lock"
cp "${PROJECT_ROOT}/requirements.txt" "${APP_BUILD_DIR}/requirements.txt"

GIT_COMMIT="$(git -C "${PROJECT_ROOT}" rev-parse HEAD)"
printf '{"git_commit":"%s"}\n' "${GIT_COMMIT}" > "${APP_BUILD_DIR}/.build-info.json"

echo "Built Databricks App source at ${APP_BUILD_DIR}"
