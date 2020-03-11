
#  process(FreeCAD.getResourceDir() + "Mod/Resources/Default.GDML")
fcn:

neo:
	/media/thomas/85e3e0e8-8016-4dda-a113-1560ebc7b48c/neo4j-desktop-offline-1.2.4-x86_64.AppImage

git:
	/usr/bin/python /usr/bin/git-cola &


# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
PAPER         =
BUILDDIR      = _build


# Internal variables.
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .
#ALLSPHINXOPTS   = -d  $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .
# the i18n builder cannot share the environment and doctrees with the others
I18NSPHINXOPTS  = $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .


cleandoc:
	rm -rf $(BUILDDIR)/*

html:
	$(SPHINXBUILD) -b html $(ALLSPHINXOPTS) $(BUILDDIR)/html
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/html."


doc:	html
	firefox  $(BUILDDIR)/html/index.html
