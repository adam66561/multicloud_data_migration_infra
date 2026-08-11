docs: 
	terraform-docs markdown table --output-file README.md --output-mode inject .

init:
	terraform init -reconfigure

plan:
	terraform fmt
	terraform validate
	terraform plan -input=false -out="terraform.planfile"

plan_destroy:
	terraform plan -destroy

apply:
	terraform apply "terraform.planfile"

destroy:
	terraform destroy -auto-approve