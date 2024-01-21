server_image:
	docker build -f Dockerfile_server -t server_image .

lb_image:
	docker build -f Dockerfile_lb -t lb_image .

client_image:
	docker build -f Dockerfile_client -t client_image .

run_lb:
	docker run --privileged -dit --name load_balancer --hostname load_balancer lb_image 

run_client:
	docker run -dit --name client --hostname client client_image