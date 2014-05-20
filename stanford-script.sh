#!/bin/bash
# script to run stanford nlp splitting and tokenization over a filelist
java -cp stanford-corenlp-3.3.1.jar:stanford-corenlp-3.3.1-models.jar:xom.jar:joda-time.jar:jollyday.jar:ejml-0.23.jar -Xmx4g edu.stanford.nlp.pipeline.StanfordCoreNLP -annotators tokenize,ssplit -tokenize.options untokenizable=noneDelete,normalizeParentheses=false,normalizeOtherBrackets =false,latexQuotes=false -filelist filelist

# post-processing, keep only the word tokens from the xml
for f in ./wiki_*xml
do
        echo "Processing $f"
        perl -pe "s/(^.*<(CharacterOffsetBegin|CharacterOffsetEnd).*$)//g;" $f > $f.ss.out	# Removes a great percentage of unnecessary markup so that...
        python xml2text.py $f.ss.out > $f.ss							# xml parse won't fail because of memory shortage
        rm $f.ss.out $f										# Remove original and intermediate xml files
done

