from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler


class StageBuilder(object):

    def __init__(self, source_df, json_args):
        """

        :param source_df:
        :param json_args:
        """
        self.source_df = source_df
        self.all_columns = source_df.columns
        self.json_args = json_args
        self.id_column = self.json_args["idColumn"]
        self.label_column = self.json_args["labelColumn"]
        self.ignored_columns = [self.label_column] + [self.id_column] + self.json_args["ignoredColumns"]
        self.categorical_columns = self.json_args["categoricalColumns"]
        self.all_used_columns = [item for item in self.all_columns if item not in self.ignored_columns]
        self.all_used_categorical_columns = [item for item in self.categorical_columns if
                                             item not in self.ignored_columns]

    def handle_categorical_features(self):
        stages = []
        # stages in our Pipeline
        for categorical_col in self.all_used_categorical_columns:
            # Category Indexing with StringIndexer
            string_indexer = StringIndexer(inputCol=categorical_col, outputCol=categorical_col + "_index")
            # Use OneHotEncoder to convert categorical variables into binary SparseVectors
            encoder = OneHotEncoder(inputCol=categorical_col + "_index", outputCol=categorical_col + "_class_vec")
            # Add stages.  These are not run here, but will run all at once later on.
            stages += [string_indexer, encoder]
        return stages

    def handle_label(self):
        label_string_idx = StringIndexer(inputCol=self.label_column, outputCol="label")
        return [label_string_idx]

    def vector_assembler(self):
        assembler_inputs = self.features()
        assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
        stages = self.handle_label()
        stages += self.handle_categorical_features()
        stages += [assembler]
        return stages

    def features(self):
        numeric_or_vector_columns = [item for item in self.all_used_columns if
                                     item not in self.all_used_categorical_columns]
        assembler_inputs = [c + "_class_vec" for c in self.all_used_categorical_columns] + numeric_or_vector_columns
     
        return assembler_inputs


class PipelineBuilder(StageBuilder):

    def __init__(self, source_df, json_args):
        """

        :param source_df:
        :param json_args:
        """
        super(PipelineBuilder, self).__init__(source_df, json_args)

    def transform(self):
        """

        :return:
        """
        all_stages = super(PipelineBuilder, self).vector_assembler()
        pipeline = Pipeline(stages=all_stages)
        pipeline_model = pipeline.fit(self.source_df)
        target_df = pipeline_model.transform(self.source_df)
        selected_columns = ["label", "features"] + [self.id_column] + [self.label_column] + self.all_used_columns
        target_df = target_df.select(selected_columns)
        return target_df
