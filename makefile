top: dcube
FNAME_A= data_A.inp
FNAME_B= data_B.inp

hw3: data_A data_B

demo:
	python q2.py

data_A:
	python q2.py $(FNAME_A)

data_B:
	python q2.py $(FNAME_B)



demo:
	@echo ""
	@echo "--- Q2 ---"
	cd Q2; make -s
	@echo ""
	@echo "--- Q3 ---"
	cd Q3; make -s
	@echo ""
	@echo "--- Q4 ---"
	cd Q4; make -s

hw3: hw3q2 hw3q3 hw3q4

hw3q2:
	@echo ""
	@echo "------- HW3 Q2 ------"
	cd Q2; make -s hw3

hw3q3:
	@echo ""
	@echo "------- HW3 Q3 ------"
	cd Q3; make -s hw3

hw3q4:
	@echo ""
	@echo "------- HW3 Q4 ------"
	cd Q4; make -s hw3

spotless:
	\rm -f *hw3.tar.gz
	cd Q2; make spotless
	cd Q3; make spotless
	cd Q4; make spotless

clean:

spotless: clean
	
dcube.tar: spotless
	tar cvf dcube.tar src docs output data makefile 
