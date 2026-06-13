# Troubleshooting

## Common Issues

### Kafka Connection Issues

**Symptom:** Producer logs show "Kafka not ready. Retrying..."

**Solution:**
- Kafka takes 30-60 seconds to start
- The producer auto-retries every 5 seconds
- Check Kafka is healthy: `docker ps` (look for "healthy" status)

**Symptom:** Cannot consume from host machine

**Solution:**
- Use `localhost:9092` for host access
- Use `kafka:29092` for Docker-internal access
- Check KAFKA_ADVERTISED_LISTENERS in docker-compose.yml

---

### Cassandra Not Ready

**Symptom:** Dashboard shows "Waiting for data..." or Spark logs show Cassandra errors

**Solution:**
- Cassandra takes 60-90 seconds to fully initialize
- The cassandra-init container must complete successfully
- Check: `docker logs smart-health-cassandra-init`
- Verify: `docker exec smart-health-cassandra cqlsh -e "DESCRIBE KEYSPACES"`

---

### Spark Model Loading Failures

**Symptom:** Spark logs show "Model file not found" warnings

**Solution:**
- Train models first: `python ml/train_models.py`
- Check models/ directory has .pkl files
- Verify volume mount in docker-compose.yml: `./models:/models:ro`
- The system will use rule-based fallback — this is not fatal

---

### Email Not Sending

**Symptom:** No emails received, logs show "Email alerting disabled"

**Solutions:**

1. **SMTP credentials not set:** Set `ALERT_SMTP_USER` in `.env`
2. **Gmail App Password:** Use App Password, not account password
   - Go to: https://myaccount.google.com/apppasswords
   - Generate a 16-character app password
3. **Recipient not set:** Set `ALERT_DOCTOR_EMAIL` to the address that should receive alerts
4. **Test from the dashboard:** Open http://localhost:5000/settings/alerts and click **Send Test Email**
5. **Test with MailHog:** No authentication needed
   ```bash
   docker compose --profile dev up -d
   # Set: ALERT_SMTP_ENABLED=true, ALERT_SMTP_HOST=mailhog, ALERT_SMTP_PORT=1025
   ```
6. **View MailHog:** http://localhost:8025

**SMTP Provider Configurations:**

| Provider | Host | Port |
|----------|------|------|
| Gmail | smtp.gmail.com | 587 |
| Outlook | smtp-mail.outlook.com | 587 |
| Yahoo | smtp.mail.yahoo.com | 587 |
| MailHog (local) | mailhog | 1025 |

---

### Dashboard Shows No Data

**Symptom:** Dashboard loads but all patients show "WAITING"

**Checklist:**
1. Is Cassandra initialized? `docker logs smart-health-cassandra-init`
2. Is the producer running? `docker logs smart-health-producer`
3. Is Spark streaming? `docker logs smart-health-spark-streaming`
4. Are there Kafka messages? Check with console consumer
5. Does `patient_latest_status` have data? Query Cassandra directly

---

### Docker Out of Memory

**Symptom:** Containers crash or restarts loop, "OOMKilled" in docker inspect

**Solution:**
- Increase Docker Desktop memory to 6GB+
- Settings → Resources → Memory → 6144 MB
- Restart Docker Desktop after changing

---

### Spark Streaming Not Processing

**Symptom:** Spark starts but no batches appear

**Checklist:**
1. Are Kafka topics created? Check kafka-setup logs
2. Is producer publishing? Check with kafka-console-consumer
3. Are packages downloaded? First run downloads Spark Kafka connector
4. Check for exceptions in Spark logs
5. Verify checkpoint directory is writable

---

### ML Training Errors

**Symptom:** `python ml/train_models.py` fails

**Common causes:**
- Missing dependencies: `pip install pandas numpy scikit-learn==1.6.1 joblib openpyxl pyyaml`
- Missing datasets: ensure `datasets/` folder has all CSV files
- xlsx file issues: install `openpyxl` for Excel support
- Permission errors: ensure write access to `models/` directory

---

### Port Conflicts

**Symptom:** "port already in use" error

**Default ports:**
- 5000: Flask Dashboard
- 8080: Spark Master UI
- 9092: Kafka (host access)
- 9042: Cassandra
- 7077: Spark Master
- 2181: Zookeeper
- 8025: MailHog Web UI
- 1025: MailHog SMTP

**Solution:** Stop other services using these ports, or modify docker-compose.yml port mappings.

---

### Rebuilding After Changes

```bash
# Rebuild specific service
docker compose build spark-streaming
docker compose up -d spark-streaming

# Rebuild everything
docker compose down
docker compose up -d --build

# Full reset (removes all data)
docker compose down -v
docker compose up -d --build
```
