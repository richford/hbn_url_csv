FROM continuumio/miniconda3

RUN conda install --yes \
    boto3==1.9.66 \
    click==7.1.2

ADD create_datalad_addurls_csv.py create_datalad_addurls_csv.py

ENTRYPOINT ["python", "create_datalad_addurls_csv.py"]
