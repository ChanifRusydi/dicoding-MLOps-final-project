List of task for this project :
- Machine Learning Model Development
 - [] Accuracy for training and testing need to be more than 92% (at least 85%)
- Requirements file
  - [] Packages and version
- 
  - [] 

Machine learning pipeline yang dibuat harus memuat seluruh komponen yang dibutuhkan seperti berikut.
- ExampleGen
- StatisticGen
- SchemaGen
- ExampleValidator
- Transform
- Trainer
- Resolver
- Evaluator
- Pusher

Seluruh komponen di atas harus dijalankan menggunakan InteractiveContext dan disimpan dalam sebuah folder bernama  chanif1516-pipeline.

Menggunakan text cell pada notebook (.ipynb) untuk menjelaskan setiap tahapan proyek dan berkas Markdown untuk menjelaskan proyek secara keseluruhan. 
- Informasi terkait dataset yang digunakan.
- Informasi tentang persoalan yang ingin diselesaikan.
- Penjelasan terkait solusi machine learning yang akan dibuat beserta target yang ingin dicapai.
- Penjelasan tentang metode pengolahan data, arsitektur model yang digunakan, dan metrik untuk mengevaluasi performa model.
- Informasi terkait performa model machine learning yang telah dibuat.

Submission file :
- [X] Notebook pembuatan pipeline .ipynb.
- [X] Module transform.py
- [X] Module trainer.py
- [X] Penjelasan proyek secara keseluruhan dalam format Markdown .md
- [X] *Optional* Notebook testing  prediction request ke model serving yang telah dibuat. .ipynb 
- [X] *Optional* Dockerfile


Folder Structure :
'''bash
Submission
|   Notebook.ipynb
|   chanif1516-testing.ipynb *Optional*
|-chanif1516-pipeline
|--SchemaGen
|--StatisticGen
|--
''''
