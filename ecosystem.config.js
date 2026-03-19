module.exports = {
  apps: [
    {
      name: "polymarket-bot",
      script: "venv/bin/python",
      args: "arb_bot.py",
      interpreter: "none",
      cwd: "/home/YOUR_USER/polymarket_arb",
      autorestart: true,
      restart_delay: 10000,   // 10s between restarts
      max_restarts: 20,
      // stdout/stderr go to ~/.pm2/logs/polymarket-bot-out.log
    },
    {
      name: "polymarket-web",
      script: "venv/bin/gunicorn",
      args: "--bind 127.0.0.1:8080 --workers 1 --timeout 30 web:app",
      interpreter: "none",
      cwd: "/home/YOUR_USER/polymarket_arb",
      autorestart: true,
      restart_delay: 5000,
    },
  ],
};
