from abc import ABCMeta

from pyspark.ml.wrapper import JavaWrapper


class Check(JavaWrapper):
    __metaclass__ = ABCMeta

    def __init__(self):
        super(Check, self).__init__()
        # self._java_obj = _jvm().com.amazon.deequ.checks.Check(_jvm()..Error,"some test")
        self._java_obj = JavaWrapper._new_java_obj("com.amazon.deequ.checks.Check",
                                                   *[JavaWrapper._new_java_obj("com.amazon.deequ.checks.CheckLevel.Error"), "some test"])

    def hasSize(self, condition):
        self._java_obj.hasSize(condition)
        return self

    def isComplete(self, column):
        self._java_obj.isComplete(column)
        return self

    def isUnique(self, column):
        self._java_obj.isComplete(column)
        return self


class VerificationSuite(JavaWrapper):
    __metaclass__ = ABCMeta

    def __init__(self):
        super(VerificationSuite, self).__init__()
        self._java_obj = JavaWrapper._new_java_obj("com.amazon.deequ.VerificationSuite")

    def onData(self, df):
        self._java_obj.onData(df._jdf)
        return self

    def addCheck(self, x: Check):
        self._java_obj.addCheck(x)
        return self

    def run(self):
        return self._java_obj.run()
