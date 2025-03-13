# InstructLab

InstructLab uses a novel synthetic data-based alignment tuning method for Large Language Models (LLMs).
The **InstructLab** sink is designed to improve the training experience with taxonomies, by taking a stream of structured data from a source like a relational database, and applying transformations to create seeded examples for a taxonomy.

## Taxonomies

In InstructLab, taxonomies define structured categories that help classify and organize knowledge or responses within the AI model.
They act like labels or tags that help the model understand and respond to different topics or domains efficiently.

Taxonomies serve several purposes:

1. They are used to guide how different pieces of knowledge are structured and retrieved.
2. They ensure that the AI provides relevant and context-aware responses.
3. They can be customized based on specific needs, allowing better control over how the AI categorizes and understands various types of questions.

## Training

The `qna.yml` file is a **Question and Answer YAML** used by InstructLab to define structured FAQ-style data.
It helps train the AI by providing predefined questions and their expected answers.

The structure of the `qna.yml` file typically contains several pieces of information:

1. A list of questions that a user might ask.
2. Various answers the AI should provide.
3. Metadata (optional) that contains additional information like categories, intents, or tags.

The `qna.yml` file therefore provides the following benefits:

1. Helps to train the AI with structured knowledge.
2. Ensures consistency with its responses.
3. Improves the AI's ability to answer domain-specific questions.

## Debezium InstructLab Integration

The Debezium Server integration is designed to provide a common framework where structured data can be passed through a series of transformations to generate a series of questions and answers.
The transformations amend the current event by adding headers, that the sink adapter consumes.
The series of questions, answers, and context details are then appended to a specific taxonomy `qna.yml` file automatically.
This eases the maintenance of these **Question and Answer YAML** files and allows driving the training data in real time from changes consumed by Debezium.

Once the taxonomy tree has been created with various `qna.yml` files, InstructLab can be told to train the model based on that knowledge.
