package pipeline

import (
	"context"

	"github.com/stellar/go/exp/ingest/io"
	supportPipeline "github.com/stellar/go/exp/support/pipeline"
)

type StatePipeline struct {
	supportPipeline.Pipeline
}

type TransactionPipeline struct {
	supportPipeline.Pipeline
}

// StateProcessor defines methods required by state processing pipeline.
type StateProcessor interface {
	// ProcessState is a main method of `StateProcessor`. It receives `io.StateReadCloser`
	// that contains object passed down the pipeline from the previous procesor. Writes to
	// `io.StateWriteCloser` will be passed to the next processor. WARNING! `ProcessState`
	// should **always** call `Close()` on `io.StateWriteCloser` when no more object will be
	// written and `Close()` on `io.StateReadCloser` when reading is finished.
	// Data required by following processors (like aggregated data) should be saved in
	// `Store`. Read `Store` godoc to understand how to use it.
	// The first argument `ctx` is a context with cancel. Processor should monitor
	// `ctx.Done()` channel and exit when it returns a value. This can happen when
	// pipeline execution is interrupted, ex. due to an error.
	//
	// Given all information above `ProcessState` should always look like this:
	//
	//    func (p *Processor) ProcessState(ctx context.Context, store *pipeline.Store, r io.StateReadCloser, w io.StateWriteCloser) error {
	//    	defer r.Close()
	//    	defer w.Close()
	//
	//    	// Some pre code...
	//
	//    	for {
	//    		entry, err := r.Read()
	//    		if err != nil {
	//    			if err == io.EOF {
	//    				break
	//    			} else {
	//    				return errors.Wrap(err, "Error reading from StateReadCloser in [ProcessorName]")
	//    			}
	//    		}
	//
	//    		// Process entry...
	//
	//    		// Write to StateWriteCloser if needed but exit if pipe is closed:
	//    		err := w.Write(entry)
	//    		if err != nil {
	//    			if err == io.ErrClosedPipe {
	//    				//    Reader does not need more data
	//    				return nil
	//    			}
	//    			return errors.Wrap(err, "Error writing to StateWriteCloser in [ProcessorName]")
	//    		}
	//
	//    		// Return errors if needed...
	//
	//    		// Exit when pipeline terminated due to an error in another processor...
	//    		select {
	//    		case <-ctx.Done():
	//    			return nil
	//    		default:
	//    			continue
	//    		}
	//    	}
	//
	//    	// Some post code...
	//
	//    	return nil
	//    }
	ProcessState(context.Context, *supportPipeline.Store, io.StateReadCloser, io.StateWriteCloser) error
	// IsConcurrent defines if processing pipeline should start a single instance
	// of the processor or multiple instances. Multiple instances will read
	// from the same StateReader and write to the same StateWriter.
	// Example: the processor can insert entries to a DB in a single job but it
	// probably will be faster with multiple DB writers (especially when you want
	// to do some data conversions before inserting).
	IsConcurrent() bool
	// RequiresInput defines if processor requires input data (StateReader). If not,
	// it will receive empty reader, it's parent process will write to "void" and
	// writes to `writer` will go to "void".
	// This is useful for processors resposible for saving aggregated data that don't
	// need state objects.
	// TODO!
	RequiresInput() bool
	// Returns processor name. Helpful for errors, debuging and reports.
	Name() string
}

// stateProcessorWrapper wraps StateProcessor to be implement pipeline.Processor interface.
type stateProcessorWrapper struct {
	StateProcessor
}

// stateReadCloserWrapper wraps StateReadCloser to be implement pipeline.ReadCloser interface.
type stateReadCloserWrapper struct {
	io.StateReadCloser
}

// readCloserWrapper wraps pipelinne.ReadCloser to be implement StateReadCloser interface.
type readCloserWrapper struct {
	supportPipeline.ReadCloser
}

// readCloserWrapper wraps pipelinne.WriteCloser to be implement StateWriteCloser interface.
type writeCloserWrapper struct {
	supportPipeline.WriteCloser
}
