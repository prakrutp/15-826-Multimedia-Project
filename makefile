make:
	@echo ""
	@echo "----- Data Preparation ------"
	cd src; python data_cleaning.py
	@echo "----- D-CUBE Run ------"
	cd src; python dcube_main.py
	
spotless:
	cd src; \rm *.pyc
	
tar:
	tar cvf marora-prakrutp-ph2.tar.gz src output data makefile README.md
