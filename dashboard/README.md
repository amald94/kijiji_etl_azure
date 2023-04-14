

<img src="../screenshot/dash.png" />

Install the required python packages
```sh
pip install -r requirements.txt
```

Start the server by running
```sh
python index.py
```
Dashboard design inspired from : [url](https://github.com/Mubeen31/Covid-19-Dashboard-in-Python-by-Plotly-Dash)

# Authenticate Docker to GCR by running the following command:
```sh
gcloud auth configure-docker
```
# Build and tag the Docker image:
```sh
docker build -t gcr.io/[PROJECT-ID]/[IMAGE-NAME] .
eq : docker build -t gcr.io/wise-isotop/kijiji-app .
```
# Finally, push the Docker image to GCR:
```sh
docker push gcr.io/[PROJECT-ID]/[IMAGE-NAME]
eq : docker push gcr.io/wise-isotope/kijiji-app
```
