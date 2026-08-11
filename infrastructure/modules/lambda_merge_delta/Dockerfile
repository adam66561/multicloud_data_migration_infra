FROM public.ecr.aws/lambda/python:3.12 AS build

COPY modules/lambda_convert_delta/requirements.txt  .

RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY modules/lambda_convert_delta/src/lambda_function.py ${LAMBDA_TASK_ROOT}

RUN dnf update -y \
    && dnf clean all \
    && rm -rf /var/cache/dnf

RUN chmod 644 $(find . -type f) && chmod 755 $(find . -type d)

HEALTHCHECK NONE

CMD [ "lambda_function.lambda_handler" ]