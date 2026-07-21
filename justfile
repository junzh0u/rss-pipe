# Deploy to remote host: sync compose file, pull latest image, restart
deploy host="dockyard" dir="rss-pipe":
    scp docker-compose.yml {{ host }}:{{ dir }}/docker-compose.yml
    ssh {{ host }} "cd {{ dir }} && docker compose pull --quiet && docker compose up -d --remove-orphans && docker image prune -f"
    ssh {{ host }} "cd {{ dir }} && docker compose ps"
