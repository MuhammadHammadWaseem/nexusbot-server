import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

// Initialize client
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

async function testConnection() {
  // Try a simple query
  const { data, error } = await supabase
    .from('user_profiles')
    .select('*')
    .limit(1);

  if (error) {
    console.error('❌ DB connection failed:', error.message);
  } else {
    console.log('✅ DB connected, sample row:', data);
  }
}

testConnection();