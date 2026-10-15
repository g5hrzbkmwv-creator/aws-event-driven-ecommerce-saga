import React, { useState } from 'react';
import OrderStatusTracker from './OrderStatusTracker';

const API_BASE_URL = 'https://hycqu1nzxl.execute-api.ap-southeast-2.amazonaws.com'; // 例如: https://xxxx.execute-api.region.amazonaws.com

export default function App() {
  const [itemsInput, setItemsInput] = useState('Apple, Banana');
  const [activeOrderId, setActiveOrderId] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // 點擊按鈕建立訂單
  const handleCreateOrder = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setActiveOrderId(null);

    try {
      // 模擬將商品字串轉成陣列
      const items = itemsInput.split(',').map(item => item.trim());
      
      const response = await fetch(`${API_BASE_URL}/orders`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          user_id: 'user_test_01',
          items: items,
        }),
      });

      if (!response.ok) {
        throw new Error('Order creation failed');
      }

      const data = await response.json();
      // 假設你的 Ingress API 回傳的 JSON 裡面包含 order_id
      const newOrderId = data.order_id;
      
      if (newOrderId) {
        setActiveOrderId(newOrderId);
      } else {
        throw new Error('order_id not found in the returned data');
      }
    } catch (err) {
      console.error(err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ padding: '40px', fontFamily: 'sans-serif', maxWidth: '600px', margin: 'auto' }}>
      <h2>🛒 Decentralized e-commerce system - Establishing order testing</h2>
      
      <form onSubmit={handleCreateOrder} style={{ background: '#f4f4f4', padding: '20px', borderRadius: '8px', marginBottom: '20px' }}>
        <div style={{ marginBottom: '10px' }}>
          <label style={{ display: 'block', marginBottom: '5px' }}>Purchase items (comma-separated):</label>
          <input
            type="text"
            value={itemsInput}
            onChange={(e) => setItemsInput(e.target.value)}
            style={{ padding: '8px', width: '100%', boxSizing: 'border-box' }}
          />
        </div>
        <button 
          type="submit" 
          disabled={loading}
          style={{ padding: '10px 20px', background: '#007bff', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
        >
          {loading ? 'Sending out...' : 'Create Order'}
        </button>
      </form>

      {error && <p style={{ color: 'red' }}>Error: {error}</p>}

      {/* 當成功建立訂單並取得 order_id 後，自動掛載輪詢追蹤器 */}
      {activeOrderId && <OrderStatusTracker orderId={activeOrderId} />}
    </div>
  );
}
