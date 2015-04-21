include ../makefile.inc

all: librbf.a rbftest1 rbftest2 rbftest3 rbftest4 rbftest5 rbftest6 rbftest7 rbftest8 rbftest8b rbftest9 rbftest10 rbftest11 rbftest12 rbftest11a rbftest11b rbftest13 rbftest14 rbftest15 rbftest16 rbftest17a rbftest17b rbftest17c

# c file dependencies
pfm.o: ../rbf/pfm.h
rbfm.o: ../rbf/rbfm.h

# lib file dependencies
librbf.a: librbf.a(../rbf/pfm.o)  # and possibly other .o files
librbf.a: librbf.a(../rbf/rbfm.o)

rbftest1.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest2.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest3.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest4.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest5.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest6.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest7.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest8.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest8b.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest9.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest10.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest11.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest11a.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest11b.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest12.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest13.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest14.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest15.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest16.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest17a.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest17b.o: ../rbf/pfm.h ../rbf/rbfm.h
rbftest17c.o: ../rbf/pfm.h ../rbf/rbfm.h



# binary dependencies
rbftest1: rbftest1.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest2: rbftest2.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest3: rbftest3.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest4: rbftest4.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest5: rbftest5.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest6: rbftest6.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest7: rbftest7.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest8: rbftest8.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest8b: rbftest8b.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest9: rbftest9.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest10: rbftest10.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest11: rbftest11.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest11a: rbftest11a.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest11b: rbftest11b.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest12: rbftest12.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest13: rbftest13.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest14: rbftest14.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest15: rbftest15.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest16: rbftest16.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest17a: rbftest17a.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest17b: rbftest17b.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest17c: rbftest17c.o librbf.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
	#-rm rbftest1 rbftest2 rbftest3 rbftest4 rbftest5 rbftest6 rbftest7 rbftest8 rbftest8b rbftest9 rbftest10 rbftest11 rbftest12 rbftest1.o *.a *.o *~ rbftest11a rbftest11b rbftest13 rbftest14 rbftest15 rbftest16 rbftest17a
clean: 
	ls | grep -v .cc | grep -v makefile | grep -v .sh | xargs rm 
