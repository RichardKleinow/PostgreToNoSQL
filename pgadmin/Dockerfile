FROM dpage/pgadmin4:latest

WORKDIR /var/lib/pgadmin

USER pgadmin
COPY --chown=pgadmin:pgadmin ./pgpass pgpass
RUN chmod 600 /var/lib/pgadmin/pgpass

WORKDIR /pgadmin4
