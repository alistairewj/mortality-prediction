# Mortality prediction

This repository contains a variety of notebooks which qualitatively and quantitatively evaluate the performance of a real time mortality prediction system developed using data from the MIMIC-III clinical database.

## Notebooks

The primary notebooks are as follows:

Notebook name | Purpose | Data used
--- | --- | ---
mp-prep-data.ipynb | Prepare various design matrices for model development | RTD, F24
mp-benchmark-model.ipynb | Benchmark machine learning models against severity of illness scores | F24
mp-random-time-evaluation.ipynb | Performance of models | RTD
mp-qualitative-evaluation | Assess the performance of models using a few test patients | RTD

Other notebooks which have tangential but interesting analyses:

Notebook name | Purpose | Data used
--- | --- | ---
mp-from-materialized-views.ipynb | Same as above, but uses pre-generated materialized views, not CSVs | RTD
mp-plot-model-risks | Plot model outputs over time on test patients | RTD
mp-plot-patient-data | Plot patient data over time | RTD

RTD: Data from a window centered at a random time during the patient's stay.
F24: Data from the first 24 hours of data.

RTD implies that the data has been extracted from a window centered at a random time during the patient's ICU stay. The goal of data extraction in this fashion is to make the model applicable at any time during a patient's ICU stay: if the distribution of data extraction was any time during an ICU stay, it is more reasonable to apply a model to any time during a patient's ICU stay. Conversely, data from the first 24 hours (F24) is more commonly used when benchmarking ICUs/hospitals. This data is meant to provide a snapshot of the patient acuity on admission, and subsequent outcomes are compared to expected outcomes to evaluate the hospital/ICU performance.

<!--
# Acknowledgement

If you use code or would like to acknowledge the work in this repository, we would be grateful if you would cite:


-->
