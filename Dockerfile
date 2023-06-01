# Copyright (C) 2022 52°North Spatial Information Research GmbH
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
#
# If the program is linked with libraries which are licensed under one of
# the following licenses, the combination of the program with the linked
# library is not considered a "derivative work" of the program:
#
#     - Apache License, version 2.0
#     - Apache Software License, version 1.0
#     - GNU Lesser General Public License, version 3
#     - Mozilla Public License, versions 1.0, 1.1 and 2.0
#     - Common Development and Distribution License (CDDL), version 1.0
#
# Therefore the distribution of the program linked with libraries licensed
# under the aforementioned licenses, is permitted by the copyright holders
# if the distribution is compliant with both the GNU General Public
# License version 2 and the aforementioned licenses.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
# Public License for more details.
#
# build with
#
#      --build-arg GIT_COMMIT=$(git rev-parse -q --verify HEAD)
#      --build-arg BUILD_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
#
# See https://wiki.52north.org/Documentation/ImageAndContainerLabelSpecification
# regarding the used labels
#
FROM python:3-slim-buster

ENV DB_HOST=
ENV DB_USER=
ENV DB_PASSWORD=
ENV DB=
ENV NO_PING=False
ENV MAX_RETRIES=15
ENV SLEEP=2
ENV PYTHONUNBUFFERED=1
ENV BASE_FOLDER=/odc
ENV DATA_FOLDER=data
ARG HOME=${BASE_FOLDER}

LABEL maintainer="Pontius, Martin <m.pontius@52north.org>" \
      org.opencontainers.image.authors="Jürrens, Eike Hinderk <e.h.juerrens@52north.org>; Pontius, Martin <m.pontius@52north.org>" \
      org.opencontainers.image.url="https://github.com/52North/TBD" \
      org.opencontainers.image.vendor="52°North GmbH" \
      org.opencontainers.image.licenses="GPL-3.0-or-later" \
      org.opencontainers.image.title="52°North Open Data Cube Importer" \
      org.opencontainers.image.description="Python toolbox for downloading and indexing data in Open Data Cube."

WORKDIR ${HOME}

COPY requirements.txt ./

RUN apt-get update \
    && apt-get install --assume-yes \
        gcc \
        libpq-dev \
        iputils-ping \
    && pip install \
            --no-cache-dir \
            --quiet \
            --disable-pip-version-check \
            --no-warn-script-location \
            --requirement requirements.txt \
    && apt-get --assume-yes purge \
        gcc \
    && apt-get autoremove --assume-yes \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir --parents --verbose ./$DATA_FOLDER


COPY ./odc_importer ./odc_importer

CMD python ./odc_importer/odc.py --host="$DB_HOST" \
                                 --user="$DB_USER" \
                                 --password="$DB_PASSWORD" \
                                 --db="$DB" \
                                 --no-ping="$NO_PING" \
                                 --sleep="$SLEEP" \
                                 --max-retries="$MAX_RETRIES"

ARG GIT_COMMIT
LABEL org.opencontainers.image.revision="${GIT_COMMIT}"

ARG BUILD_DATE
LABEL org.opencontainers.image.created="${BUILD_DATE}"

ARG VERSION=latest
ARG IMG_REF=52north/opendatacube-importer
LABEL org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.ref.name="${IMG_REF}:${VERSION}"
