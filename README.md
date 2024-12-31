# Order-Management-Site

Overview
This project implements a real-time Order Management System (OMS) using Redis, MongoDB, and Python. Designed for efficiency and scalability, the system ensures rapid order processing, dynamic inventory updates, and insightful analytics for modern e-commerce platforms.

Features
 - Real-Time Order Processing: Utilizes Redis Streams to handle continuous incoming orders.
 - Inventory Management: Real-time stock adjustments with Redis Sets and Lists, with low-stock alerts via Pub/Sub.
 - Analytics & Insights: Tracks top-ordered items and inventory trends using Redis Sorted Sets.
 - Alert System: Sends email notifications for low-stock items.
 - Data Persistence: Achieved with Redis RDB snapshots and AOF logging for reliability.

Technologies Used
 - Redis: In-memory database for ultra-fast data operations.
 - MongoDB: Primary data store for order details.
 - Python: Backend logic, including inventory updates and alerting mechanisms.

Contributions
 - Mariam Abidi: Managed Redis Streams, order tracking, and analytics.
 - Suhas Vittal: Handled inventory uploads, stock updates and state management.
