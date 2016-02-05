package com.gaea.tailsource.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

public class Objects {

	private ExpressHandler defaultHandler;

	private Object ref = null;

	private boolean result = false;
	
	private Object _result_data = null;
	
	private Objects(Object ref) {
		this.ref = ref;
	}

	public final static Objects startChain(Object ref) {
		return new Objects(ref);
	}

	public final <E> Objects orInstanceof(Class<E> clz) {
		if (!result) {
			if (clz.isInstance(ref)) {
				this.result = defaultHandler.doHandler(clz);
			}
		}
		return this;
	}

	public final <E> Objects orInstanceof(Class<E> clz, ExpressHandler handler) {
		checkNotNull(handler, "handler should not be null.");
		if (!result) {
			this.result = handler.doHandler(clz);
		}
		return this;
	}

	public final Objects withInstanceofDefaultHandler(ExpressHandler handler) {
		checkNotNull(handler, "handler should not be null.");
		defaultHandler = handler;
		return this;
	}

	public final Objects withRetryHandler(int retryInterval, int maxInterval, int maxRetry, boolean returnResult, 
			ExpressHandler handler) throws InterruptedException {
		checkNotNull(handler, "handler should not be null.");
		int retry = 0;
		while (true) {
			try {
				if (retry >= maxRetry) {
					System.err.println("Has retried more than max retry times.");
					break;
				}
				this.result = handler.doHandler(this.ref);
				if (returnResult){
					this._result_data = handler.getData();
				}
				break;
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Unexpected failure, try again after "
						+ retryInterval + " ms");
				TimeUnit.MILLISECONDS.sleep(retryInterval);
				retryInterval = retryInterval << 1;
				retryInterval = Math.min(retryInterval, maxInterval);
				retry++;
				continue;
			}
		}
		return this;
	}

	public final boolean result() {
		return this.result;
	}
	
	public Object getResultData(){
		if (!result()){
			return null;
		}
		return this._result_data;
	}
	
	public static abstract class ExpressHandler {
		private Object _data = null;

		public ExpressHandler() {
		}

		public ExpressHandler(Object o) {
			_data = o;
		}

		public Object getData() {
			return _data;
		}

		public void setDate(Object data) {
			this._data = data;
		}

		public abstract <E> boolean doHandler(E clz);
	}

	public final static class DefaultHandler extends ExpressHandler {
		@Override
		public <E> boolean doHandler(E clz) {
			System.out.println(((Class<?>)clz).getName() + " orInstanceof error occured!");
			return false;
		}

	}

	public static void main(String[] args) {

	}

}
