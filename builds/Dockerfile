# import phase 3 container
FROM callahantiff/pheknowlator:latest

# allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# copying local scripts to container
COPY *.py *.txt /PheKnowLator/builds/

# set wroking directory in container
WORKDIR /PheKnowLator/builds/

# install requirements
RUN pip install -r build_requirements.txt
EXPOSE 8080
#ENTRYPOINT ["python", "build_phase_3.py"]
ENTRYPOINT ["gunicorn", "--bind", ":$PORT", "--workers", "1", "--threads", "8", "--timeout", "0", "main:app"]