
-  Instale dependências:

```python
pip install flask pymysql
pip install python-dotenv

```

```python
C:\Users\geral>curl -X POST http://127.0.0.1:5001/start
{"error":"(1045, \"Access denied for user 'root'@'localhost' (using password: NO)\")","ok":false}
```

```sql
CREATE TABLE `email_outbox` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `to_email` varchar(255) NOT NULL,
  `subject` varchar(255) NOT NULL,
  `body_html` longtext,
  `body_text` mediumtext,
  `status` int NOT NULL DEFAULT '0',
  `attempts` int NOT NULL DEFAULT '0',
  `last_error` text,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `sent_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_email_outbox_status` (`status`,`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE INDEX idx_email_outbox_status ON email_outbox(status, id);


INSERT INTO email_outbox (`to_email`, `subject`, `body_html`, `body_text`) VALUES ('geraldo.filho92@gmail.com', 'subject', 'body1', 'body2');

```