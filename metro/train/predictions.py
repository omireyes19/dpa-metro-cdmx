from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier

class predictions:
    def get_predictions(self, X_train, y_train):

        categorical_features = X_train.select_dtypes(include=['object']).columns
        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
            ('onehot', OneHotEncoder(handle_unknown='ignore'))])

        preprocessor = ColumnTransformer(
            transformers=[
                ('cat', categorical_transformer, categorical_features)])

        rf = Pipeline(steps=[('preprocessor', preprocessor),
                             ('classifier', RandomForestClassifier())])

        model = rf.fit(X_train, y_train)

        return model
