import os
os.environ['OPENBLAS_NUM_THREADS'] = '1'

import autosklearn.regression
import sklearn.metrics
import pandas as pd

if __name__ == "__main__":

    f = open("stats-Average23cb031d5--1593549630.csv")
    df = pd.read_csv(f, sep=";", header=0)
    f.close()
    df = df._get_numeric_data()
    print(df)
    numeric_headers = list(df.columns.values)
    #print(numeric_headers)
    #X, y = sklearn.datasets.load_digits(return_X_y=True)
    X, y = df.drop("e2eLatencyMean", axis=1).drop("e2eLatencyStdDev", axis=1), df["e2eLatencyMean"]

    X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(X, y, random_state=1)

    automl = autosklearn.regression.AutoSklearnRegressor(
        time_left_for_this_task=120,
        per_run_time_limit=30,
        tmp_folder='/tmp/autosklearn_regression_example_tmp',
        output_folder='/tmp/autosklearn_regression_example_out',
    )

    automl.fit(X_train, y_train)

    y_hat = automl.predict(X_test)

    print("R2 score:", sklearn.metrics.r2_score(y_test, y_hat))
    print("models: \n", automl.show_models())
