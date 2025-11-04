# services/jupyter/Dockerfile
FROM jupyter/base-notebook:python-3.11

USER root
RUN apt-get update &&     apt-get install -y --no-install-recommends postgresql-client awscli &&     rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt ||     pip install --no-cache-dir duckdb psycopg2-binary

# == pg_dump_16_begin ==
# Install PostgreSQL client 16 from PGDG (matches server v16)
USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends wget gnupg ca-certificates; \
    install -d -m 0755 /usr/share/keyrings; \
    wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc \
      | gpg --dearmor \
      | tee /usr/share/keyrings/postgresql.gpg >/dev/null; \
    . /etc/os-release && echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends postgresql-client-16; \
    rm -rf /var/lib/apt/lists/*; \
    apt-get clean
USER $NB_UID
# == pg_dump_16_end ==





ENV PYTHONPATH=/work
USER ${NB_USER}
CMD ["start-notebook.sh", "--NotebookApp.token=''"]
