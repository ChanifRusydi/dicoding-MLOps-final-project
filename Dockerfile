FROM tensorflow/serving:2.15.1
COPY serving_model/sentiment-model/ /models/sentiment-model
ENV MODEL_BASE_PATH=/models
ENV MODEL_NAME=sentiment-model
ENTRYPOINT ["/usr/bin/tf_serving_entrypoint.sh"]
# RUN echo '#!/bin/bash \n\n\
# env \n\
# tensorflow_model_server --port=8500 --rest_api_port=${PORT} \
# --model_name=${MODEL_NAME} --model_base_path=${MODEL_BASE_PATH}/${MODEL_NAME} \
# "$@"' > /usr/bin/tf_serving_entrypoint.sh \
# && chmod +x /usr/bin/tf_serving_entrypoint.sh  