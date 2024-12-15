
import tensorflow as tf

LABEL_KEY = "sentiment"
FEATURE_KEY = "review"

def preprocessing_fn(inputs):
    outputs = {}
    LABEL_NAME = LABEL_KEY + "_xf"
    FEATURE_NAME = FEATURE_KEY + "_xf"
    outputs[FEATURE_NAME] = tf.strings.lower(inputs[FEATURE_KEY])
    outputs[LABEL_NAME] = tf.cast(inputs[LABEL_KEY], tf.int64)
    
    return outputs
