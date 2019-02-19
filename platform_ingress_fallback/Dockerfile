FROM nginx:1.15

COPY entrypoint.sh /
COPY html /usr/share/nginx/html

ENTRYPOINT ["/entrypoint.sh"]

CMD ["nginx", "-g", "daemon off;"]
