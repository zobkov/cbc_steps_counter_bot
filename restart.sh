systemctl daemon-reload
systemctl restart cbc_steps_counter_bot
journalctl -u cbc_steps_counter_bot.service -f