library(pROC)

args = commandArgs(trailingOnly=TRUE)
print(args)

#fns = Sys.glob('C:/Users/sm/Documents/Temp/roc/Clin*.csv')
fns = Sys.glob(paste0(args[[1]],'*.csv'))
print(paste0(args[[1]],'*.csv'))
print(fns)
targets = list(list('s', c(0)))#, list('s', 1))

doRoc <- function(labels, predictions) {
	par(pty = "s")
	rocPlot <- roc(labels, predictions,
				smoothed = TRUE,
				# arguments for ci
				ci=TRUE, ci.alpha=0.9, stratified=FALSE,
				# arguments for plot
				plot=T, auc.polygon=TRUE, max.auc.polygon=TRUE,
				print.auc=TRUE, show.thres=TRUE, quiet=T)
	print(rocPlot$auc)
}

graphics.off()
for (fn in fns) {
	d <- read.csv(fn)
	for (target in targets) {
		png(paste0(dirname(fn),'/',target[[1]],'_',target[[2]],'.',basename(fn),'.roc.png'), width=900, height=900, pointsize=34)
		dTemp <- d
		#doRoc(sum(dTemp$trueState == target[[2]])>0, Reduce(function(a,v) a+v, lapply(target[[2]],function(x)dTemp[[paste0('state',x)]])))
		doRoc(dTemp$trueState == target[[2]], dTemp[[paste0('state',target[[2]])]])
		graphics.off()
	}
}