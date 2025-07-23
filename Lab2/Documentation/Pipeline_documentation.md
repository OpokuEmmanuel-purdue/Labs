{
  "nbformat": 4,
  "nbformat_minor": 0,
  "metadata": {
    "colab": {
      "provenance": [],
      "toc_visible": true
    },
    "kernelspec": {
      "name": "python3",
      "display_name": "Python 3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "cells": [
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "id": "-RMW6Tg33znQ"
      },
      "outputs": [],
      "source": []
    },
    {
      "cell_type": "markdown",
      "source": [
        "#                        My Dataflow Pipeline Documentation\n",
        "\n",
        "## Pipeline Overview\n",
        "- **Purpose:** To process Superstore sales data from a CSV file, transform it into a structured format, and load it into a BigQuery table for analysis.\n",
        "- **Source:** A CSV file (`Sample - Superstore.csv`) located in a Google Cloud Storage bucket.\n",
        "- **Transformations:**\n",
        "  - Reads the source CSV file, handling `latin-1` encoding to prevent common decoding errors.\n",
        "  - Parses each line, converting specific columns to the correct data types (INTEGER, FLOAT, DATE).\n",
        "  - Formats date strings from `M/D/YYYY` to the BigQuery-compatible `YYYY-MM-DD` format.\n",
        "  - Catches and logs any rows that are malformed or cause a processing error, then filters them out.\n",
        "- **Destination:** A BigQuery table named `superstore_transformed`.\n",
        "\n",
        "## Pipeline Configuration\n",
        "- **Job name:** `mgmt599-emmanuel-pipeline`\n",
        "- **Region:** `us-central1`\n",
        "- **Machine type:** [Not specified, uses Dataflow default]\n",
        "- **Max workers:** [Not specified, uses Dataflow autoscaling]\n",
        "\n",
        "## Data Flow\n",
        "1. **Read from:** `gs://mgmt599-emmanuel-opoku-data-lake/pipeline_input/Sample - Superstore.csv`\n",
        "2. **Transform:** The pipeline parses each CSV row, converts date and number formats, and filters out any rows that fail validation to prevent the job from failing due to bad data.\n",
        "3. **Write to:** `mgmt599-emmanuel-opoku-lab1:pipeline_processed_data.superstore_transformed`\n",
        "\n",
        "## Lessons Learned\n",
        "- **What was challenging?**\n",
        "  - **Character Encoding:** The source file was not standard UTF-8, requiring a custom `Latin1Coder` to read the data without errors. This is a common issue when dealing with files from different systems.\n",
        "  - **Data-Type Mismatches:** The raw CSV data had dates and numbers stored as strings. Explicit conversion to `DATE`, `INTEGER`, and `FLOAT` was necessary to match the BigQuery schema and enable proper analysis.\n",
        "  - **Error Handling:** The source data contained malformed rows. Implementing a `try-except` block was essential to gracefully skip these rows and log them, rather than letting the entire pipeline fail.\n",
        "- **What would you do differently?**\n",
        "  - **Dead-Letter Queue:** Instead of just logging and dropping bad records, I would route them to a separate BigQuery \"dead-letter\" table. This would allow for later inspection and reprocessing of failed records.\n",
        "  - **Parameterization:** I would replace the hardcoded GCS paths and BigQuery table names with `PipelineOptions` to make the pipeline more reusable and configurable from the command line.\n",
        "\n"
      ],
      "metadata": {
        "id": "JrUy8k_P4okV"
      }
    }
  ]
}