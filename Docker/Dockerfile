FROM erlang:19.2

#install confd to this file
COPY /confd-0.10.0-linux-amd64 /usr/local/bin/confd
RUN chmod +x /usr/local/bin/confd

#add confd templates to some path
COPY ./confd/ /etc/confd/


#copy rel-compiles antidote to docker context (same folder as Dockerfile)
COPY antidote  /antidote/

COPY entrypoint.sh /entrypoint.sh

WORKDIR  /



RUN chmod +x /antidote/bin/antidote*
RUN chmod +x /entrypoint.sh

ENV NODE_NAME_CMD "sname changeme"
ENV CLUSTER_MASTER_NAME "changeme"

CMD "./entrypoint.sh"
