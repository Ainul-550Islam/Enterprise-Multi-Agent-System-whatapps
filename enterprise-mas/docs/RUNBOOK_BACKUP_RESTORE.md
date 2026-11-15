# Backup and Restore Runbook

## Backup Procedure

### 1. Database Backup

```bash
# PostgreSQL dump
pg_dump -Fc -U mas_user -d enterprise_mas > backup-$(date +%Y%m%d).dump

# MinIO/S3 backup
mc cp backup-$(date +%Y%m%d).dump minio/mas-backups/

# Or AWS CLI
aws s3 cp backup-$(date +%Y%m%d).dump s3://mas-backups/$(date +%Y%m%d)/
```

### 2. Runtime State Backup

```bash
# mas data directory
tar czf state-$(date +%Y%m%d).tar.gz   /var/lib/mas/data/

# mas configuration
tar czf config-$(date +%Y%m%d).tar.gz   /etc/mas/
```

### 3. Verification

```bash
# Verify backup integrity
pg_restore -l backup-$(date +%Y%m%d).dump | head -5

# Verify mas CLI can read backup
mas status
```
```

### 2. Restore Procedure

```bash
# Restore PostgreSQL
pg_restore -d enterprise_mas backup-$(date +%Y%m%d).dump

# Restart mas services
systemctl restart mas-api mas-worker mas-sandbox

# Verify
mas status
```

### 3. Post-Restore Checklist

- [ ] mas CLI can connect and authenticate
- [ ] All agents are accessible
- [ ] API keys are valid
- [ ] Observability metrics are flowing
- [ ] Backups are verified and stored off-site
