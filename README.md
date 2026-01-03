Secure Sales Analytics Pipeline (Python + AWS S3)
Overview

This project is a Python-based data processing pipeline that ingests a sales CSV file, performs analytics using Pandas, generates derived reports, and securely uploads results to AWS S3.

The design intentionally emphasizes secure configuration management, least-privilege cloud interaction, and clean data handling, making it a strong example of security-aware Python development.

Features

📊 Reads and processes structured CSV sales data using Pandas

🗓️ Calculates Tuesday-only sales and determines a weekly top seller

📈 Generates an aggregate sales dashboard for downstream consumption (e.g., web apps)

☁️ Uploads generated reports to AWS S3

🧹 Automatically removes local artifacts after upload

🔐 Uses environment variables for all sensitive configuration
