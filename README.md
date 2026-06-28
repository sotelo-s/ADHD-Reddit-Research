# ADHD Reddit Research
Attention Deficit Hyperactivity Disorder Detection based on Narratives

[![TFM](https://img.shields.io/badge/TFM-PDF-blue)](./Detection_of_Personality_Disorders_based_on_Narratives.pdf)

This repository contains the code and resources for the Master's thesis: **"Detection of Personality Disorders based on Narratives"**.

## Abstract
While computational methods have been widely applied to detect linguistic markers of mental health conditions such as depression and anxiety, Attention Deficit Hyperactivity Disorder (ADHD) has received comparatively little attention. This TFM addresses this gap by introducing a novel multimodal corpus of social media texts from individuals with ADHD and neurotypical controls. Using this resource, we conduct a comparative linguistic analysis and establish a set of baselines across three classification paradigms: Bag-of-Words, fine-tuned BERT, and zero-shot multimodal LLaVA-NeXT. Performance improves as we increase the model’s complexity, yet all variants achieved limited overall performance. Different analyses show that users with ADHD posted more frequently, showed higher negative affect, and expressed less positive sentiment.

**Keywords:** Attention Deficit Hyperactivity Disorder, social media, Reddit, natural language processing, language models, text classification, multimodal analysis, BERT, corpus construction, linguistic markers

## TFM

The full manuscript is available here: [Detection_of_Personality_Disorders_based_on_Narratives.pdf](./Detection_of_Personality_Disorders_based_on_Narratives.pdf)


## Contributions

1. Construction of a labeled corpus of ADHD and control social media texts and images
2. Analysis of linguistic features identified in the corpus
3. Comparative evaluation of three classification paradigms:
   - Traditional Bag-of-Words (BoW) classifier
   - Fine-tuned BERT model
   - Multimodal zero-shot classifier

## Image Repository

The image repository is available here: [Image Repository](https://nubeusc-my.sharepoint.com/:f:/g/personal/sabrina_sotelo_rai_usc_es/IgCNhdlA_5aKSZy-TEFzY9AkAbEeDloX6NEtYm3xPT-_2J4?e=UJsidT)

## Repository Structure

```
├── code/
│ ├── crawler/ # Reddit data collection
│ │ ├── crawler.py
│ │ └── data/
│ │   ├── adhd_phrases.json
│ │   └── adhd_search_subreddits.json
│ │
│ ├── data_processing/ # Data cleaning & preprocessing
│ │ ├── join_files.py
│ │ ├── data_cleaning.py
│ │ ├── update_adhd.py
│ │ ├── media_anonymizer.py
│ │ ├── nsfw_censoring.py
│ │ └── upload_preparation.py
│ │
│ ├── training/ # Classification experiments
│ │ ├── bag_of_words.py
│ │ ├── BERT_fine_tuning.py
│ │ └── multimodal_zeroshot.py
│ │
│ └── analysis/ # Analysis & visualization
│   ├── corpus_view.ipynb
│   ├── experiment_result.ipynb
│   └── data/
│     ├── experiment_results.csv
│     └── predictions/
│
├── datasets/ # Anonymized corpus
│ ├── users.csv
│ └── content.csv
│
├── Detection_of_Personality_Disorders_based_on_Narratives.pdf
└── README.md
```