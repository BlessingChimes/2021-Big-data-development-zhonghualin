if __name__ == "__main__":
    import pandas as pd

    pd.set_option("display.max_columns", None)

    #  下面是对拿到的原始数据进行预处理，由于有很多缺失值，
    #  我们对数据进行四舍五入后计算众数，使用众数的平均值来填充
    df = pd.read_csv("./water_potability.csv")

    print(df.head(10))

    print(df['ph'].value_counts())
    print(df['Hardness'].value_counts())
    print(df['Solids'].value_counts())
    print(df['Chloramines'].value_counts())
    print(df['Sulfate'].value_counts())
    print(df['Conductivity'].value_counts())
    print(df['Organic_carbon'].value_counts())
    print(df['Trihalomethanes'].value_counts())
    print(df['Turbidity'].value_counts())
    print(df['Potability'].value_counts())

    #  对于数值，均使用所在列的四舍五入后的值的众数进行
    df['ph'] = df['ph'].fillna(df['ph'].round().mode().mean())
    df['Hardness'] = df['Hardness'].fillna(df['Hardness'].round().mode().mean())
    df['Solids'] = df['Solids'].fillna(df['Solids'].round().mode().mean())
    df['Chloramines'] = df['Chloramines'].fillna(df['Chloramines'].round().mode().mean())
    df['Sulfate'] = df['Sulfate'].fillna(df['Sulfate'].round().mode().mean())
    df['Conductivity'] = df['Conductivity'].fillna(df['Conductivity'].round().mode().mean())
    df['Organic_carbon'] = df['Organic_carbon'].fillna(df['Organic_carbon'].round().mode().mean())
    df['Trihalomethanes'] = df['Trihalomethanes'].fillna(df['Trihalomethanes'].round().mode().mean())
    df['Turbidity'] = df['Turbidity'].fillna(df['Turbidity'].round().mode().mean())
    df['Potability'] = df['Potability'].fillna(df['Potability'].round().mode().mean())

    print(df['ph'].round().mode().mean())

    df.to_csv("./preprocessing.csv", index=False)

    # df = pd.read_csv("./preprocessing.csv")
    # df = df.loc[ : , ~df.columns.str.contains("^Unnamed")]
    print(df.head(10))

    #  由于特征值都为数字，因此无需再进行额外的处理，开始使用随机森林拟合并探究特征的重要性
    y = df.get("Potability")
    X = df.drop("Potability", axis=1)

    from sklearn.ensemble import RandomForestRegressor

    rf = RandomForestRegressor()
    rf.fit(X, y)

    feature_importances = rf.feature_importances_
    feature_importance_pairs = [(feature_name, feature_importance)
                                for feature_name, feature_importance in
                                zip(X.columns, feature_importances)]

    feature_importance_pairs = sorted(feature_importance_pairs, key=lambda x: x[1], reverse=True)
    feature_importance_names = [name[0] for name in feature_importance_pairs]
    feature_importance_vals = [name[1] for name in feature_importance_pairs]

    import matplotlib.pyplot as plt
    figure = plt.figure()
    plt.bar(range(len(feature_importance_names)), feature_importance_vals, orientation='vertical')
    plt.xticks(range(len(feature_importance_names)), feature_importance_names, rotation='vertical')
    plt.show()

    #  经过上述作图，我们可以发现对于结果来说，九个特征均有一定的重要性，因此我们使用所有九个特征进行预测
    from sklearn.model_selection import train_test_split

    train_X, test_X, train_y, test_y = train_test_split(X, y, test_size=0.25, random_state=114514)

    rf = RandomForestRegressor()
    model = rf.fit(train_X, train_y)

    import joblib
    joblib.dump(model, "./rf.pkl", compress=3)

    model = joblib.load("./rf.pkl")
    pred_y = model.predict(test_X)

    #  测试发现选取阈值为0.65时指标表现较好
    threshold = 0.65
    for i in range(len(pred_y)):
        if pred_y[i] >= threshold:
            pred_y[i] = 1
        else:
            pred_y[i] = 0

    #  模型评估
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
    accuracy_score_value = accuracy_score(test_y, pred_y)
    print(f"准确率:{accuracy_score_value}")

    precision_score_value = precision_score(test_y, pred_y)
    print(f"精确率:{precision_score_value}")

    recall_score_value = recall_score(test_y, pred_y)
    print(f"召回率:{recall_score_value}")

    f1_score_value = f1_score(test_y, pred_y)
    print(f"f1值:{f1_score_value}")

    #  将预测结果写入S3存储
    pred_y_all = model.predict(X)
    for i in range(len(pred_y_all)):
        if pred_y_all[i] >= threshold:
            pred_y_all[i] = 1
        else:
            pred_y_all[i] = 0

    X['Potability'] = y
    X['predict'] = pred_y_all
    X.to_csv("./predict.csv", index=False)

    import boto3
    ACCESS_KEY = "B67DDB9A1DCDE1F208B4"
    SECRET_KEY = "WzZGQ0MwOUEwNTlFNjI2RjgwMTkzQUZERkIwRDgy"
    serviceEndpoint = "http://scut.depts.bingosoft.net:29997"
    bucketName = 'luna'
    fileName = "predict.csv"

    print("Uploading result of predicting...")
    s3 = boto3.client('s3',
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY,
                      endpoint_url=serviceEndpoint)
    s3.upload_file(fileName, bucketName, fileName)
    print("Upload success!")


