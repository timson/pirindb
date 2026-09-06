package storage

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}
	return append([]byte(nil), data...)
}

func (tx *Tx) ensureOpen() error {
	if tx == nil {
		return ErrTxClosed
	}
	if tx.beginErr != nil {
		return tx.beginErr
	}
	if tx.db == nil || tx.closed {
		return ErrTxClosed
	}
	return nil
}

func (tx *Tx) ensureWritable() error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if !tx.write {
		return ErrWriteInRxTransaction
	}
	return nil
}

func (bucket *Bucket) ensureOpen() error {
	if bucket == nil || bucket.tx == nil {
		return ErrTxClosed
	}
	return bucket.tx.ensureOpen()
}

func (bucket *Bucket) ensureWritable() error {
	if bucket == nil || bucket.tx == nil {
		return ErrTxClosed
	}
	return bucket.tx.ensureWritable()
}
